import sys
from producer import send_log
from consumer import consume_logs

def main():
    if len(sys.argv) != 2:
        print("Usage: python app.py [producer|consumer]")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == 'producer':
        send_log()
    elif mode == 'consumer':
        consume_logs()
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
