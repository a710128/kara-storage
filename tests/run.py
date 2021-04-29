import os, sys

def check_cmd(cmd):
    v = os.system(cmd)
    if v != 0:
        print(v)
        sys.exit(v)

def main():
    path = os.path.dirname(os.path.abspath(__file__))
    check_cmd("python3 %s" % os.path.join(path, "write.py"))
    check_cmd("python3 %s" % os.path.join(path, "read.py"))
    check_cmd("python3 %s" % os.path.join(path, "data_loader.py"))
    check_cmd("python3 %s" % os.path.join(path, "http.py"))

if __name__ == "__main__":
    main()
