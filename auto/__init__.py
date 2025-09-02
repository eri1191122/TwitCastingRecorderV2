#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Auto Recording Package for TwitCasting Recorder V2
- 5本同時録画対応
- 20人監視対応
"""

__version__ = "2.0.0"

# 公開API
from .job_queue import JobQueue, RecordJob, JobStatus, PriorityQueue
from .recorder_wrapper import RecorderWrapper

__all__ = [
    "JobQueue",
    "RecordJob",
    "JobStatus",
    "PriorityQueue",
    "RecorderWrapper",
]