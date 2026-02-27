#!/usr/bin/env python3
"""Spawn Phase 2 workers for personal account to process the 700 unclaimed IDs."""
import os
from src.worker.celery_app import celery_app
from src.worker.id_first_tasks import fetch_message_batch
from celery import group

# Personal account ID
account_id = '8f28b22f-cc5c-46c3-9114-1d8551192fa7'

# Spawn 7 Phase 2 workers (700 IDs / 100 batch size = 7 workers)
print(f"Spawning 7 Phase 2 workers for personal account...")

tasks = [fetch_message_batch.s(account_id) for _ in range(7)]
job = group(tasks)
result = job.apply_async()

print(f"\n✅ Spawned 7 workers to process 700 personal account emails")
print(f"Workers will claim and process batches of 100 IDs each")
