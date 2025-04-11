import time
import psutil
import os

# Config
log_file_path = r"../App_Details/steam_app_scraper.log"  # Replace with actual path
target_phrase = "Processing appID:"
target_pid = 17332  # Replace with the actual PID of the process to kill

def monitor_log_and_kill():
    print(f"Watching log: {log_file_path}")
    try:
        with open(log_file_path, "r", encoding="utf-8", errors="ignore") as f:
            # Move to the end of the file
            f.seek(0, os.SEEK_END)

            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.2)
                    continue

                print("LOG:", line.strip())
                if target_phrase in line:
                    print(f"Found '{target_phrase}' – killing process {target_pid}")
                    try:
                        psutil.Process(target_pid).terminate()
                        print(f"Process {target_pid} terminated.")
                    except psutil.NoSuchProcess:
                        print(f"Process {target_pid} not found.")
                    break
    except Exception as e:
        print(f"Error: {e}")

monitor_log_and_kill()
