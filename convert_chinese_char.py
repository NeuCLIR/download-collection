import argparse
import json
from pathlib import Path

from tqdm.auto import tqdm
import chinese_converter

def convert_text(text, to="traditional"):
    return getattr(chinese_converter, f"to_{to}")(text)

def main(args):
    if not args.document_file.exists():
        raise FileNotFoundError(f"Input file {args.document_file} does not exsit.")

    output_fn = args.document_file.parent / f"{args.document_file.stem}.{args.convert_to}.jsonl"
    if output_fn.exists():
        raise FileExistsError(f"Output file {output_fn} already exists, "
                              "please delete or rename it if you want to rerun the convertion.")

    if args.line_count is None:
        total_lines = None
    elif args.line_count == "infer":
        total_lines = sum(1 for _ in tqdm(args.document_file.open(), desc="counting lines") )
    else:
        total_lines = int(args.line_count)

    with output_fn.open("w") as fw, args.document_file.open() as fr:
        for line in tqdm(fr, total=total_lines):
            line = json.loads(line)
            line['title'] = convert_text(line['title'], to=args.convert_to)
            line['text'] = convert_text(line['text'], to=args.convert_to)
            fw.write( json.dumps(line) + "\n" )
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Convert Chinese characters in docs.jsonl")
    parser.add_argument('--document_file', type=Path, help="Path to the document .jsonl file.")
    parser.add_argument('--convert_to', choices=['traditional', 'simplified'],
                        help="Chinese character set to convert to. Note that this script only transform the characters "
                             "not the terminologies.")
    parser.add_argument('--line_count', type=str, default=None,
                        help="Line count of the document set. Use `infer` if you would like to count the line first. "
                             "Providing an integer would be treated as the number of lines.")

    main(parser.parse_args())
