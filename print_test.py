
# test_escpos.py
import win32print, time

PRINTER = r"GP-80220(Cut) Series" # <-- EXACT Windows name

def send_raw(data: bytes) -> bool:
    h = None
    try:
        h = win32print.OpenPrinter(PRINTER)
        job = ("ESC/POS test", None, "RAW")
        win32print.StartDocPrinter(h, 1, job)
        win32print.StartPagePrinter(h)
        win32print.WritePrinter(h, data)
        win32print.EndPagePrinter(h)
        win32print.EndDocPrinter(h)
        return True
    except Exception as e:
        print("ERROR:", e)
        return False
    finally:
        if h:
            try: win32print.ClosePrinter(h)
            except: pass

# Minimal ESC/POS:
ESC_INIT = b"\x1b\x40" # Initialize
CENTER = b"\x1b\x61\x01" # Align center
LEFT = b"\x1b\x61\x00"
CUT = b"\x1d\x56\x42\x00" # Full cut (may be \x1d\x56\x00 on some)
FEED6 = b"\n"*6

payload = (
    ESC_INIT +
    CENTER + b"SCHOOL MANAGER TEST\n" +
    LEFT + b"Student: John Doe\n" +
    b"Amount : 10,000 UGX\n" +
    b"Method : Cash\n" +
    FEED6 + CUT
)

ok = send_raw(payload)
print("Sent:", ok)
time.sleep(1)
