import argparse

def bind_row(parser : argparse.ArgumentParser):
    parser.add_argument("url", help="KARA Storage location", type=str)
    parser.add_argument("action", help="actions: view dataset", type=str, choices=["view"])
    parser.add_argument("namespace", help="namespace", type=str)
    parser.add_argument("key", help="key", type=str)
    parser.add_argument("-v", "--version", type=str, default=None, help="version")
    parser.add_argument("--begin", help="beginning index of dataset", type=int, default=0)
    parser.add_argument("--app-key", type=str, default=None, help="OSS app key")
    parser.add_argument("--app-secret", type=str, default=None, help="OSS app secret")

    from ..funcs import handle_row
    parser.set_defaults(func=handle_row)