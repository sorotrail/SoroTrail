# API Keys and Authentication

SoroTrail supports secure API key authentication across both PostgreSQL and SQLite backends.

## SQLite Single-Node Authentication

When deploying SoroTrail with SQLite (`DATABASE_URL=sqlite:./sorotrail.db`), API key authentication is fully supported. If no API keys are configured, SoroTrail logs a loud startup warning reminding operators to secure the deployment or run behind an authenticated reverse proxy.