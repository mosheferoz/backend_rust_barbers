import io
import os
import time
from pathlib import Path

import requests
from cachetools import TTLCache
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from jose import jwt
from jose.exceptions import JWTError
from PIL import Image

load_dotenv()

app = FastAPI(title="Instagram Posts API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = "instagram-scraper21.p.rapidapi.com"
BASE_URL = f"https://{RAPIDAPI_HOST}/api/v1"

HEADERS = {
    "Content-Type": "application/json",
    "x-rapidapi-host": RAPIDAPI_HOST,
    "x-rapidapi-key": RAPIDAPI_KEY,
}

FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID")
FIREBASE_CERTS_URL = (
    "https://www.googleapis.com/robot/v1/metadata/x509/"
    "securetoken@system.gserviceaccount.com"
)

# Cache Firebase public keys for 1 hour (TTL in seconds).
_firebase_keys_cache: TTLCache = TTLCache(maxsize=1, ttl=3600)

# Rate limiter: uid -> list of request timestamps (monotonic seconds since epoch).
_rate_limit_state: dict[str, list[float]] = {}
_RATE_LIMIT_WINDOW_SECONDS = 600  # 10 minutes
_RATE_LIMIT_MAX_REQUESTS = 3

TEMPLATES_DIR = Path(__file__).parent / "templates"


def _get_firebase_public_keys() -> dict[str, str]:
    """Fetch Firebase x509 public certs with 1-hour TTL caching."""
    cached = _firebase_keys_cache.get("certs")
    if cached is not None:
        return cached
    resp = requests.get(FIREBASE_CERTS_URL, timeout=10)
    if resp.status_code != 200:
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    keys = resp.json()
    _firebase_keys_cache["certs"] = keys
    return keys


def verify_firebase_token(request: Request) -> str:
    """Verify a Firebase ID token and return the Firebase uid.

    Reads `Authorization: Bearer <token>` from request headers, validates the
    RS256 signature against Google's public certs, checks `aud`, `iss`, `exp`,
    and `sub` claims, and returns the `sub` (uid). On any failure raises 401.
    """
    if not FIREBASE_PROJECT_ID:
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    auth_header = request.headers.get("Authorization") or request.headers.get("authorization")
    if not auth_header or not auth_header.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    token = auth_header.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    try:
        unverified_header = jwt.get_unverified_header(token)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    kid = unverified_header.get("kid")
    if not kid:
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    try:
        certs = _get_firebase_public_keys()
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    cert_pem = certs.get(kid)
    if not cert_pem:
        # Key might have rotated since last cache fill; try a forced refresh once.
        _firebase_keys_cache.clear()
        try:
            certs = _get_firebase_public_keys()
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid or missing token")
        cert_pem = certs.get(kid)
        if not cert_pem:
            raise HTTPException(status_code=401, detail="Invalid or missing token")

    expected_issuer = f"https://securetoken.google.com/{FIREBASE_PROJECT_ID}"

    try:
        claims = jwt.decode(
            token,
            cert_pem,
            algorithms=["RS256"],
            audience=FIREBASE_PROJECT_ID,
            issuer=expected_issuer,
            options={"verify_at_hash": False},
        )
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    # Defense-in-depth: jose already validates exp/aud/iss, but re-check explicitly.
    now = int(time.time())
    exp = claims.get("exp")
    if not isinstance(exp, (int, float)) or exp <= now:
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    if claims.get("aud") != FIREBASE_PROJECT_ID:
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    if claims.get("iss") != expected_issuer:
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    uid = claims.get("sub")
    if not uid or not isinstance(uid, str):
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    return uid


def _check_rate_limit(uid: str) -> None:
    """Enforce 3 requests per uid per 10 minutes. Raises 429 if exceeded."""
    now = time.time()
    cutoff = now - _RATE_LIMIT_WINDOW_SECONDS
    timestamps = _rate_limit_state.get(uid, [])
    timestamps = [t for t in timestamps if t > cutoff]
    if len(timestamps) >= _RATE_LIMIT_MAX_REQUESTS:
        _rate_limit_state[uid] = timestamps
        raise HTTPException(
            status_code=429,
            detail="Too many import attempts. Try again in 10 minutes.",
        )
    timestamps.append(now)
    _rate_limit_state[uid] = timestamps


@app.get("/", response_class=HTMLResponse)
def index():
    return (TEMPLATES_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/profile/{username}")
def get_profile(username: str):
    """Get Instagram profile info."""
    resp = requests.get(f"{BASE_URL}/info", params={"id_or_username": username}, headers=HEADERS)
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Failed to fetch profile")

    data = resp.json()
    if data.get("status") != "ok" or not data.get("data"):
        raise HTTPException(status_code=404, detail="Profile not found")

    user = data["data"]["user"]
    return {
        "username": user.get("username"),
        "full_name": user.get("full_name"),
        "biography": user.get("biography"),
        "profile_pic_url": user.get("profile_pic_url"),
        "profile_pic_hd": (user.get("hd_profile_pic_url_info") or {}).get("url"),
        "follower_count": user.get("follower_count"),
        "following_count": user.get("following_count"),
        "media_count": user.get("media_count"),
        "is_private": user.get("is_private"),
        "is_verified": user.get("is_verified"),
        "category": user.get("category"),
        "bio_links": [link.get("url") for link in (user.get("bio_links") or [])],
    }


@app.get("/posts/{username}")
def get_posts(username: str):
    """Get profile info and all posts for an Instagram username."""
    profile_resp = requests.get(f"{BASE_URL}/info", params={"id_or_username": username}, headers=HEADERS)
    posts_resp = requests.get(f"{BASE_URL}/posts", params={"username": username}, headers=HEADERS)

    # profile
    profile = None
    if profile_resp.status_code == 200:
        profile_data = profile_resp.json()
        if profile_data.get("status") == "ok" and profile_data.get("data"):
            user = profile_data["data"]["user"]
            profile = {
                "username": user.get("username"),
                "full_name": user.get("full_name"),
                "biography": user.get("biography"),
                "profile_pic_url": user.get("profile_pic_url"),
                "profile_pic_hd": (user.get("hd_profile_pic_url_info") or {}).get("url"),
                "follower_count": user.get("follower_count"),
                "following_count": user.get("following_count"),
                "media_count": user.get("media_count"),
                "is_private": user.get("is_private"),
                "is_verified": user.get("is_verified"),
                "category": user.get("category"),
                "bio_links": [link.get("url") for link in (user.get("bio_links") or [])],
            }

    # posts
    if posts_resp.status_code != 200:
        raise HTTPException(status_code=posts_resp.status_code, detail="Failed to fetch posts from Instagram API")

    data = posts_resp.json()
    if data.get("status") != "ok":
        raise HTTPException(status_code=502, detail=data.get("message", "API error"))

    posts = data.get("data", {}).get("posts", [])
    result = []
    for post in posts:
        images = post.get("image") or []
        videos = post.get("video") or []
        result.append({
            "code": post.get("code"),
            "url": f"https://www.instagram.com/p/{post.get('code')}/",
            "caption": post.get("caption", ""),
            "date": post.get("taken_at_human_readable"),
            "timestamp": post.get("taken_at"),
            "image_url": images[0]["url"] if images else None,
            "video_url": videos[0]["url"] if videos else None,
            "is_video": len(videos) > 0,
            "all_images": [img["url"] for img in images[:3]],
            "all_videos": [vid["url"] for vid in videos[:2]],
        })

    return {"profile": profile, "count": len(result), "posts": result}


ALLOWED_HOSTS = ["cdninstagram.com", "instagram.com", "fbcdn.net", "scontent"]


def _is_instagram_url(url: str) -> bool:
    return any(host in url for host in ALLOWED_HOSTS)


@app.get("/proxy")
def proxy_image(url: str):
    """Proxy an Instagram image to avoid CORS issues in the browser."""
    if not _is_instagram_url(url):
        raise HTTPException(status_code=400, detail="Only Instagram URLs are allowed")
    resp = requests.get(url, stream=True)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Failed to fetch image")
    content_type = resp.headers.get("Content-Type", "image/jpeg")
    return StreamingResponse(resp.iter_content(chunk_size=8192), media_type=content_type)


@app.get("/download")
def download_media(media_url: str):
    """Download a specific image or video by URL."""
    if not _is_instagram_url(media_url):
        raise HTTPException(status_code=400, detail="Only Instagram media URLs are allowed")

    resp = requests.get(media_url, stream=True)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Failed to download media")

    content_type = resp.headers.get("Content-Type", "application/octet-stream")
    ext = "mp4" if "video" in content_type else "jpg"

    return StreamingResponse(
        resp.iter_content(chunk_size=8192),
        media_type=content_type,
        headers={"Content-Disposition": f"attachment; filename=instagram_media.{ext}"},
    )


@app.post("/collage")
def create_collage(
    images: list[UploadFile] = File(...),
    size: int = 1080,
    gap: int = 0,
):
    """Create a 2x2 collage from exactly 4 uploaded images."""
    if len(images) != 4:
        raise HTTPException(status_code=400, detail="Exactly 4 images are required")

    cell_size = (size - gap) // 2
    collage = Image.new("L", (size, size), color=0)

    positions = [
        (0, 0),
        (cell_size + gap, 0),
        (0, cell_size + gap),
        (cell_size + gap, cell_size + gap),
    ]

    for img_file, (x, y) in zip(images, positions):
        img = Image.open(img_file.file)
        # crop to square from center
        w, h = img.size
        side = min(w, h)
        left = (w - side) // 2
        top = (h - side) // 2
        img = img.crop((left, top, left + side, top + side))
        img = img.resize((cell_size, cell_size), Image.LANCZOS)
        img = img.convert("L")
        collage.paste(img, (x, y))

    buf = io.BytesIO()
    collage.save(buf, format="JPEG", quality=95)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="image/jpeg",
        headers={"Content-Disposition": "attachment; filename=collage.jpg"},
    )


def _shape_profile(user: dict) -> dict:
    """Map an Instagram user payload to the onboarding profile shape."""
    bio = (user.get("biography") or "").strip()
    if len(bio) > 500:
        bio = bio[:500]

    profile_pic = (user.get("hd_profile_pic_url_info") or {}).get("url")
    if not profile_pic:
        profile_pic = user.get("profile_pic_url")

    website = None
    for link in user.get("bio_links") or []:
        url = (link or {}).get("url")
        if url:
            website = url
            break

    return {
        "username": user.get("username"),
        "shop_name": user.get("full_name"),
        "bio": bio,
        "profile_pic_url": profile_pic,
        "website": website,
        "is_private": bool(user.get("is_private")),
        "category": user.get("category"),
    }


def _shape_gallery(posts: list[dict], limit: int = 20) -> list[dict]:
    """Filter to non-video posts and map to the gallery shape."""
    gallery: list[dict] = []
    for post in posts:
        videos = post.get("video") or []
        is_video = len(videos) > 0
        if is_video:
            continue
        images = post.get("image") or []
        if not images:
            continue
        first_image = images[0]
        image_url = first_image.get("url") if isinstance(first_image, dict) else None
        if not image_url:
            continue
        gallery.append({
            "image_url": image_url,
            "caption": post.get("caption") or "",
            "date": post.get("taken_at_human_readable"),
            "is_video": False,
        })
        if len(gallery) >= limit:
            break
    return gallery


@app.get("/onboarding/import/{username}")
def onboarding_import(username: str, uid: str = Depends(verify_firebase_token)):
    """Fetch an Instagram profile + gallery shaped for the onboarding flow.

    Requires a valid Firebase ID token in `Authorization: Bearer <token>`.
    Rate limited to 3 requests per uid per 10 minutes.
    """
    _check_rate_limit(uid)

    info_resp = requests.get(
        f"{BASE_URL}/info", params={"id_or_username": username}, headers=HEADERS
    )
    if info_resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Failed to fetch Instagram profile")

    info_data = info_resp.json()
    if info_data.get("status") != "ok" or not info_data.get("data"):
        raise HTTPException(
            status_code=404,
            detail=f"Instagram account not found: {username}",
        )

    user = info_data["data"].get("user") or {}
    profile = _shape_profile(user)

    if profile["is_private"]:
        return {"profile": profile, "gallery": [], "gallery_count": 0}

    gallery: list[dict] = []
    try:
        posts_resp = requests.get(
            f"{BASE_URL}/posts", params={"username": username}, headers=HEADERS
        )
        if posts_resp.status_code != 200:
            print(
                f"[onboarding_import] /posts returned {posts_resp.status_code} for {username}"
            )
        else:
            posts_data = posts_resp.json()
            if posts_data.get("status") != "ok":
                print(
                    f"[onboarding_import] /posts status != ok for {username}: "
                    f"{posts_data.get('message')}"
                )
            else:
                posts = posts_data.get("data", {}).get("posts", []) or []
                gallery = _shape_gallery(posts, limit=20)
    except Exception as exc:  # noqa: BLE001
        print(f"[onboarding_import] /posts fetch failed for {username}: {exc}")

    return {
        "profile": profile,
        "gallery": gallery,
        "gallery_count": len(gallery),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
