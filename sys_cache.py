import os
import base64

# === Configuration ===
MAX_USES = 15
SECRET_KEY = 0x6A  # XOR key
USAGE_FILE = os.path.join(os.getcwd(), "test_counter.tmp")  # Hidden file in user home
SUPPORT_CONTACT = "https://github.com/KSKH"  # Change this to your real contact

def xor_encrypt_decrypt(data, key):
    return bytes([b ^ key for b in data])

def save_usage(count):
    raw = str(count).encode()
    encrypted = xor_encrypt_decrypt(raw, SECRET_KEY)
    encoded = base64.b64encode(encrypted)

    with open(USAGE_FILE, 'wb') as f:
        f.write(encoded)

def load_usage():
    if not os.path.exists(USAGE_FILE):
        return 0

    try:
        with open(USAGE_FILE, 'rb') as f:
            encoded = f.read()
        encrypted = base64.b64decode(encoded)
        decrypted = xor_encrypt_decrypt(encrypted, SECRET_KEY)
        return int(decrypted.decode())
    except Exception:
        # Tampered or unreadable
        return MAX_USES + 1

def check_usage():
    count = load_usage()
    if count >= MAX_USES:
        print("\n❌ Trial expired.")
        print(f"📩 Please contact support to continue: {SUPPORT_CONTACT}")
        print(f"🗂️ Usage tracking file: {USAGE_FILE}\n")
        input("Press Enter to exit...")
        exit()

    count += 1
    save_usage(count)
    print(f"✅ Usage {count}/{MAX_USES}")

# Call this at the beginning of your app
if __name__ == "__main__":
    check_usage()
