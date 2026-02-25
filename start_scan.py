#!/usr/bin/env python3
"""
Start a Gmail scan across all configured accounts.

Usage:
    python start_scan.py
"""

import asyncio
import os
import sys

import httpx


async def start_scan():
    """Start a new Gmail scan."""
    api_url = os.getenv(
        "APP_URL",
        "https://gmail-obsidian-sync-729716d2143d.herokuapp.com"
    )
    user_id = os.getenv("USER_ID", "d4475ca3-0ddc-4ea0-ac89-95ae7fed1e31")

    payload = {
        "user_id": user_id,
        "account_labels": ["procore-main", "procore-private", "personal"],
    }

    print(f"🚀 Starting Gmail scan...")
    print(f"   API: {api_url}")
    print(f"   User: {user_id}")
    print(f"   Accounts: {', '.join(payload['account_labels'])}")
    print()

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{api_url}/scan/start",
                json=payload,
            )

            if response.status_code == 200:
                data = response.json()
                job_id = data.get("job_id")
                print(f"✅ Scan started successfully!")
                print(f"   Job ID: {job_id}")
                print()
                print(f"Monitor progress with: ./monitor_scan.sh")
                return 0
            else:
                print(f"❌ Failed to start scan")
                print(f"   Status: {response.status_code}")
                print(f"   Response: {response.text}")
                return 1

    except Exception as e:
        print(f"❌ Error starting scan: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(start_scan()))
