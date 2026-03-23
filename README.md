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
- `/api/account/me`
- `/api/account/token-transactions`
- `/api/billing/create-checkout-session`
- `/api/webhooks/stripe`
- `/api/social/accounts`
- `/api/social/accounts` (DELETE - remove current user's integration profile)
- `/api/social/connect-url`
- `/api/social/publish-video`
- `/api/social/publish-status`
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
- `UPLOAD_POST_API_BASE_URL`
- `UPLOAD_POST_API_KEY`
- `NANO_BANANO_PUBLIC_BASE_URL` or `PUBLIC_APP_URL`
- `FRONTEND_APP_URL`
- `FRONTEND_ORIGINS`
- `TOKEN_INITIAL_BALANCE`
- `TOKEN_COST_PHOTO`
- `TOKEN_COST_VIDEO`
- `STRIPE_SECRET_KEY`
- `STRIPE_WEBHOOK_SECRET`
- `STRIPE_TOKEN_PRICE_CENTS`
- `STRIPE_TOKEN_PRICE_USD` (supports fractional token price, e.g. `0.096`)
- `STRIPE_PACKAGE_PRICE_CENTS` (package map like `200:1900,320:3000,540:5000`)
- `STRIPE_CHECKOUT_SUCCESS_URL` (example: `http://localhost:5173/billing?checkout=success&session_id={CHECKOUT_SESSION_ID}`)
- `STRIPE_CHECKOUT_CANCEL_URL` (example: `http://localhost:5173/billing?checkout=cancel`)
