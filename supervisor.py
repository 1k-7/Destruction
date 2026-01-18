import subprocess
import sys
import time
import os
import signal
import shutil

# Configuration
BOT_SCRIPT = "main.py"
HEARTBEAT_FILE = "bot_heartbeat.tmp"
MAX_HEARTBEAT_AGE = 30   # Aggressive: if no heartbeat for 30s, kill it
POLL_INTERVAL = 0.1      # Check status every 100ms (near instant)

def cleanup_cache():
    """Clears local SQLite session files instantly for a fresh start."""
    print("[Supervisor] 🔥 Clearing session cache...")
    for file in os.listdir("."):
        if file.endswith(".session") or file.endswith(".session-journal"):
            try:
                os.remove(file)
            except Exception:
                pass # Fail silent for speed

def run_bot():
    while True:
        # Clear heartbeat before starting to reset timer
        if os.path.exists(HEARTBEAT_FILE):
            try: os.remove(HEARTBEAT_FILE)
            except: pass
            
        print(f"[Supervisor] 🚀 Launching {BOT_SCRIPT}...")
        
        # Start the bot process
        process = subprocess.Popen([sys.executable, BOT_SCRIPT])
        
        # The Monitor Loop
        while True:
            # 1. Check if process exited (Crash or /restart)
            retcode = process.poll()
            if retcode is not None:
                print(f"[Supervisor] ⚠️ Bot exited with code {retcode}.")
                # Exit code 1 = Requested via /restart command
                if retcode == 1: 
                    cleanup_cache()
                # BREAK immediately to restart (No sleep here)
                break
            
            # 2. Hang Detection (Heartbeat)
            if os.path.exists(HEARTBEAT_FILE):
                try:
                    mtime = os.path.getmtime(HEARTBEAT_FILE)
                    if (time.time() - mtime) > MAX_HEARTBEAT_AGE:
                        print("[Supervisor] 💀 Bot is hung! Force killing...")
                        process.kill() # Kill immediately, no mercy
                        cleanup_cache()
                        break
                except:
                    pass # File might be locked/deleted, ignore
            
            # Tiny sleep to prevent 100% CPU usage on the supervisor itself
            time.sleep(POLL_INTERVAL)
            
        # Loop restarts immediately here. No delay.

if __name__ == "__main__":
    # Handle supervisor shutdown
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    run_bot()
