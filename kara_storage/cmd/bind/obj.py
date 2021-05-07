import argparse

def bind_obj(parser : argparse.ArgumentParser):
    parser.add_argument("url", help="KARA Storage location", type=str)
    parser.add_argument("action", help="actions: load or save directory", type=str, choices=["load", "save"])
    parser.add_argument("namespace", help="namespace", type=str)
    parser.add_argument("key", help="key", type=str)
    parser.add_argument("-v", "--version", type=str, default=None, help="version")
    parser.add_argument("path", help="local path", type=str)

    parser.add_argument("--app-key", type=str, default=None, help="OSS app key")
    parser.add_argument("--app-secret", type=str, default=None, help="OSS app secret")

    from ..funcs import handle_obj
    parser.set_defaults(func=handle_obj)
