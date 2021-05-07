import kara_storage

def handle_obj(args):
    kwargs = {}
    if args.app_key is not None:
        kwargs["app_key"] = args.app_key
    if args.app_secret is not None:
        kwargs["app_secret"] = args.app_secret
    storage = kara_storage.KaraStorage(args.url, **kwargs)
    
    if args.action == "load":
        if args.version is None:
            args.version = "latest"
        storage.load_directory(args.namespace, args.key, args.path, args.version)
    elif args.action == "save":
        storage.save_directory(args.namespace, args.key, args.path, args.version)
    