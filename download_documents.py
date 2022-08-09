"""
This script is modified from https://github.com/complementizer/wcep-mds-dataset/blob/master/dataset_reproduction/extract_wcep_articles.py
"""

import argparse
import sys
import logging
import json
import gzip
from pathlib import Path

from collections import defaultdict
from contextlib import contextmanager
from multiprocessing import Pool, Lock
from functools import partial
from tqdm.auto import tqdm

import requests
import newspaper
from warcio.archiveiterator import ArchiveIterator

from fix_document_order import hash_doc

CHINESE = 'zho'
RUSSIAN = 'rus'
PERSIAN = 'fas'

# When adding a new language, fill in argument dict below as well
LANGUAGES = [CHINESE, RUSSIAN, PERSIAN]
LANG_NAME = {CHINESE: 'Chinese', RUSSIAN : 'Russian', PERSIAN : 'Persian'}

file_lock = Lock()
@contextmanager
def write_lock(fn, mode):
    file_lock.acquire()
    f = open(fn, mode)
    try:
        yield f
    finally:
        f.flush()
        f.close()
        file_lock.release()

def read_warc_gz(cc_file, cc_base_url="https://data.commoncrawl.org/"):
    url = cc_base_url + cc_file
    resp = requests.get(url, stream=True)
    for record in ArchiveIterator(resp.raw, arc2warc=True):
        # if (record.rec_type == 'response' and \
        #     record.http_headers.get_header('Content-Type') == 'text/html'):
        if record.content_type == 'application/http; msgtype=response':
            rid = record.rec_headers.get_header('WARC-Record-ID')\
                  .split('uuid:')[1].split('>')[0]
            yield rid, record

def extract_article(record):
    html = record.content_stream().read()
    url = record.rec_headers.get_header('WARC-Target-URI')
    extracted = newspaper.Article("", fetch_images=False)

    extracted.download(input_html=html)
    extracted.parse()

    # time = None if extracted.publish_date is None else extracted.publish_date.isoformat()
    # short hand for above
    time = extracted.publish_date and extracted.publish_date.isoformat()

    return {
        'time': time,
        'title': extracted.title,
        'text': extracted.text,
        'url': url,
    }


def process_cc_file(info, out_paths, validate, disable_tqdm, retry=10, saving=True,
                    cc_base_url="https://data.commoncrawl.org/"):
    cc_file, want_idx = info
    saved_docs = defaultdict(list)

    success = False
    for ntried in range(retry):
        try:
            pbar = tqdm(disable=disable_tqdm, total=len(want_idx))
            found_idx = set()
            for rid, record in read_warc_gz(cc_file, cc_base_url=cc_base_url):
                if rid in want_idx:
                    doc = {
                        'id': rid,
                        'cc_file': cc_file,
                        **extract_article(record)
                    }

                    for lang_used in want_idx[rid]:
                        got_hash = hash_doc(doc)
                        if want_idx[rid][lang_used] is not None and want_idx[rid][lang_used] != got_hash:
                            if validate:
                                raise AssertionError(f"md5 hash not matched in {lang_used}")
                            logging.warning(f'[hash-mismatch] record-id: {rid}, {lang_used}, expecting {want_idx[rid][lang_used]} but got {got_hash}')
                        else: 
                            logging.info(f'[hash-matched] record-id: {rid}, {lang_used}')
                        if saving:
                            saved_docs[lang_used].append(doc)

                    found_idx.add(rid)
                    pbar.update()

                if len(found_idx) == len(want_idx):
                    logging.info(f"Found all needed docs in {cc_file}, early stopping")
                    break

            if validate:
                assert len(found_idx) == len(want_idx), f"Not finding all needed docs in {cc_file}"

            success = True
            break

        except AssertionError:
            logging.warning(f"Assertion error, retrying {ntried+1} times.")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logging.warning(f"Connection error {e} on {cc_file}, retrying {ntried+1} times.")
        finally:
            pbar.close()

    if success:
        if saving: 
            for lang, docs in saved_docs.items():
                with write_lock(out_paths[lang], 'a') as fw:
                    for d in docs:
                        fw.write(json.dumps(d, ensure_ascii=False) + '\n')

        logging.info(f'done-cc-file:{cc_file}')


def read_doc_file(path):
    return set(
        json.loads(doc)['id']
        for doc in tqdm(open(path), desc=f'Reading downloaded document file from {path}')
    )

def mute_other_loggers():
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('PIL').setLevel(logging.WARNING)
    logging.getLogger('newspaper').setLevel(logging.WARNING)
    logging.getLogger('chardet.charsetprober').setLevel(logging.WARNING)
    logging.getLogger('chardet.universaldetector').setLevel(logging.WARNING)
    logging.getLogger('jieba').setLevel(logging.CRITICAL)
    logging.getLogger('bs4.dammit').setLevel(logging.ERROR)

def main(args):
    if args.restart and args.resume:
        raise ValueError("Cannot restart and resume at the same time.")

    # arguments for the languages
    lang_id_file = {
        lang: getattr(args, lang)
        for lang in LANGUAGES if getattr(args, lang) is not None
    }

    storage = Path(args.storage)
    storage.mkdir(exist_ok=True, parents=True)

    local_rank_tag = "" if args.rank == -1 else f"{args.rank}."

    logpath = storage / f'download_log.{local_rank_tag}txt'
    if logpath.exists() and args.restart:
        logpath.unlink()

    logging.basicConfig(
        level=logging.DEBUG,
        filename=logpath,
        filemode=('w' if not args.resume else 'a'),
        format='%(asctime)s %(levelname)-8s [%(name)s] %(message)s'
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    mute_other_loggers()

    out_paths = {}
    for lang in lang_id_file:
        out_paths[lang] = storage / lang / f'docs.{local_rank_tag}jsonl'
        out_paths[lang].parent.mkdir(exist_ok=True, parents=True)
        if out_paths[lang].exists():
            if args.restart:
                logging.warning(f"{out_paths[lang]} exists, will delete for restart.")
                out_paths[lang].unlink()
            elif not args.resume:
                raise FileExistsError(f"File {out_paths[lang]} already exists.")

    if len(out_paths) == 0:
        raise ValueError("No languages to process.")

    downloaded_doc_ids = defaultdict(dict)
    for lang in lang_id_file.keys():
        if out_paths[lang].exists():
            downloaded_doc_ids[lang] = read_doc_file(out_paths[lang])
            logging.info(f"Resuming -- already downloaded {len(downloaded_doc_ids[lang])} {lang} docs.")


    logging.info(f'building dictionaries of document to capture')

    # Dict[cc_file, Dict[id, Dict[langs, hashs] ] ]
    to_capture = defaultdict(lambda : defaultdict(dict))
    for lang, id_files in lang_id_file.items():
        for id_file in tqdm(id_files, desc=f'building dict for {lang}', disable=args.rank>-1):
            fp = gzip.open(id_file) if id_file.endswith('.gz') else open(id_file)
            for line in tqdm(fp, desc=f'{lang} -- {id_file}', leave=False, disable=args.rank>-1):
                line = json.loads(line)
                if line['id'] not in downloaded_doc_ids[lang]:
                    md5 = line['md5'] if 'md5' in line else None
                    to_capture[ line['cc_file'] ][ line['id'] ][ lang ] = md5

    logging.info(f'Looking for {sum(len(idx) for idx in to_capture.values())} '
                 f'documents in {len(to_capture)} cc_files')

    if len(to_capture) == 0:
        raise ValueError("No documents need to be captured.")

    worker_ = partial(process_cc_file, out_paths=out_paths, validate=args.check_hash, 
                      disable_tqdm=(args.jobs>1 or args.rank>-1), retry=args.retry,
                      saving=(not args.no_save), cc_base_url=args.cc_base_url)
    it = to_capture.items() 
    if args.limit > 0:
        it = map(lambda p: p[0], zip(it, range(args.limit+1)))
    if args.jobs > 1:
        with Pool(args.jobs) as pool:
            list(pool.imap_unordered(
                worker_,
                tqdm(it, desc="All files")
            ))
    elif args.total_rank > 1 and args.rank > -1:
        list(map(worker_, 
            map(lambda p: p[1], 
                filter(lambda x: x[0]%args.total_rank == args.rank, enumerate(it) )
            )
        ))
    else:
        list(map(worker_, tqdm(it, desc="All files")))

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Download documents from CC.")
    parser.add_argument('--storage', required=True,
                        help='Directory for storing document jsonl files.')
    for lang in LANGUAGES:
        parser.add_argument('--'+lang, nargs='+',
                            help=f'File containing {LANG_NAME[lang]} ids.')
    parser.add_argument('--jobs', type=int, default=4, help='Number of processes.')
    parser.add_argument('--restart', action='store_true', default=False,
                        help='Restart download from scratch.')
    parser.add_argument('--retry', type=int, default=20, 
                        help='Number of retries per CC file when downloading.')
    parser.add_argument('--resume', action='store_true', default=False,
                        help="Resume download.")

    parser.add_argument('--check_hash', action='store_true', default=False,
                        help="Validate document hashes during download.")

    parser.add_argument('--cc_base_url', type=str, default="https://data.commoncrawl.org/",
                        help="The base URL for CC WARC files.")

    # for github actions
    parser.add_argument('--no_save', action='store_true', default=False)
    parser.add_argument('--rank', type=int, default=-1)
    parser.add_argument('--total_rank', type=int, default=1)
    parser.add_argument('--limit', type=int, default=-1)

    main(parser.parse_args())

