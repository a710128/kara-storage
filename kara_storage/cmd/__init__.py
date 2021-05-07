import argparse
from .bind import bind_obj, bind_row

def get_parser():
    parser = argparse.ArgumentParser(prog="kara_storage", description="KARA Storage command line interface.")
    parser.set_defaults(func=None)
    sub_parsers = parser.add_subparsers(help="storage types")
    
    bind_obj(sub_parsers.add_parser("obj", help="object storage"))
    bind_obj(sub_parsers.add_parser("object", help="object storage"))
    bind_row(sub_parsers.add_parser("row", help="row storage"))
    return parser


def main():
    parser = get_parser()

    args = parser.parse_args()
    if args.func is None:
        parser.print_help()
    else:
        args.func(args)