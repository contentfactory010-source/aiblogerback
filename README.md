# AI Influencer 2

## Structure
- `../frontend` - React + Vite + React Router
- `.` - Python FastAPI backend

## Backend Run
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 -m uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

## Frontend Run
```bash
cd ../frontend
npm install
npm run dev
```

## API
- `/api/bloggers`
- `/api/bloggers/:id`
- `/api/bloggers/:id/create-in-nano`
- `/api/bloggers/:id/looks`
- `/api/bloggers/:id/assets`
- `/api/videos`
- `/api/videos/:id`
- `/api/upload`
- `/api/upload-video`
- `/api/trend-videos`

Static files: `/uploads/*`
Data: `backend/data/*.json`

## Env
Use `backend/.env`:
- `NANO_BANANO_API_KEY`
- `NANO_BANANO_BASE_URL`
- `NANO_BANANO_VEO_BASE_URL`
- `KIE_FILE_UPLOAD_BASE_URL`
- `NANO_BANANO_CALLBACK_URL`
- `UPLOAD_API_BASE_URL`
- `NANO_BANANO_PUBLIC_BASE_URL` or `PUBLIC_APP_URL`
- `FRONTEND_ORIGINS`
