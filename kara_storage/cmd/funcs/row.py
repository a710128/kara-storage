import kara_storage
import blessed
import signal
import json

widths = [
    (126,    1), (159,    0), (687,     1), (710,   0), (711,   1), 
    (727,    0), (733,    1), (879,     0), (1154,  1), (1161,  0), 
    (4347,   1), (4447,   2), (7467,    1), (7521,  0), (8369,  1), 
    (8426,   0), (9000,   1), (9002,    2), (11021, 1), (12350, 2), 
    (12351,  1), (12438,  2), (12442,   0), (19893, 2), (19967, 1),
    (55203,  2), (63743,  1), (64106,   2), (65039, 1), (65059, 0),
    (65131,  2), (65279,  1), (65376,   2), (65500, 1), (65510, 2),
    (120831, 1), (262141, 2), (1114109, 1),
]
 
def char_width( o ):
    global widths
    if o == 0xe or o == 0xf:
        return 0
    for num, wid in widths:
        if o <= num:
            return wid
    return 1

def handle_row(args):
    
    kwargs = {}
    if args.app_key is not None:
        kwargs["app_key"] = args.app_key
    if args.app_secret is not None:
        kwargs["app_secret"] = args.app_secret
    storage = kara_storage.KaraStorage(args.url, **kwargs)
    ds = storage.open_dataset(
        args.namespace, 
        args.key,
        mode="r", 
        version="latest" if args.version is None else args.version,
        buffer_size = 128 * 1024
    )
    ds.seek( args.begin )

    term = blessed.Terminal()

    
    idx_ = args.begin
    with term.fullscreen(), term.cbreak(), term.hidden_cursor():
        view_port_offx = 0
        view_port_offy = 0
        text = json.dumps(ds.read(), indent="    ", ensure_ascii=False).splitlines(keepends=False)

        def draw(*args):
            row = term.height
            col = term.width

            with term.location(0, 0):
                top_bar = " Index: %d" % idx_
                print( term.black_on_darkkhaki( top_bar + (" " * (col - len(top_bar))) ) )

            with term.location(0, 1):
                for i in range(row - 2):
                    if i + view_port_offx < len(text) and i + view_port_offx >= 0:
                        line = text[i + view_port_offx]
                        st_pos = 0
                        st_idx = 0
                        while st_idx < len(line) and st_pos < view_port_offy:
                            st_pos += char_width( ord(line[st_idx]) )
                            st_idx += 1
                        if st_idx >= len(line):
                            print(" " * col)
                            continue
                        st_pos -= view_port_offy
                        print_line = " " * st_pos
                        while st_idx < len(line) and st_pos + char_width( ord(line[st_idx]) ) < col:
                            print_line += line[st_idx]
                            st_pos += char_width( ord(line[st_idx]) )
                            st_idx += 1
                        print_line += " " * (col - st_pos)
                        print(print_line)
                    else:
                        print(" " * col)

            with term.location(0, row - 1):
                hint_line = "[N]: Next          [T]: To the top    [B]: To the bottom [Q]: Quit"
                print( term.black_on_darkkhaki( hint_line + (" " * (col - len(hint_line))) ) , end="")

        draw()
        signal.signal(signal.SIGWINCH, draw)
        while True:
            inp = term.inkey()
            if inp.is_sequence:
                if inp.name.lower() == "key_left":
                    view_port_offy += 2
                    draw()
                elif inp.name.lower() == "key_right":
                    view_port_offy -= 2
                    draw()
                elif inp.name.lower() == "key_up":
                    view_port_offx += 1
                    draw()
                elif inp.name.lower() == "key_down":
                    view_port_offx -= 1
                    draw()
            else:
                if inp.lower() == "q":
                    break
                elif inp.lower() == "n":
                    try:
                        text = json.dumps(ds.read(), indent="    ", ensure_ascii=False).splitlines(keepends=False)
                    except EOFError:
                        break
                    view_port_offx = 0
                    view_port_offy = 0
                    idx_ += 1
                    draw()
                elif inp.lower() == "t":
                    view_port_offx = 0
                    view_port_offy = 0
                    draw()
                elif inp.lower() == "b":
                    view_port_offx = len(text) - (term.height - 2)
                    view_port_offy = 0
                    draw()
                

