import psutil

def get_pid_by_name(name):
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info.get('cmdline') or []
            if name.lower() in (proc.info['name'] or "").lower() or \
               any(name.lower() in cmd.lower() for cmd in cmdline):
                return proc.info['pid']
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return None

pid = get_pid_by_name("getAppDetails")  # Replace with your target process keyword
if pid:
    print(f"Found PID: {pid}")
else:
    print("Process not found.")
