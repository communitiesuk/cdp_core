import sys
import importlib


def main():
    try:
        module = importlib.import_module(sys.argv[1])
        func = getattr(module, "execute")
        func(sys.argv[2])
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()