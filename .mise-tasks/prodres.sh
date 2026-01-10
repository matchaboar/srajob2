#!/usr/bin/env bash
#MISE description="Run worker in prod without resetting schedules or queue"
SKIP_PREFLIGHT_CHECKS=1 ./start_worker.ps1 -ResetProcessingQueue:$false -UseProd
