# NeuCLIR Collection 1

**If you are registered as a participant of the TREC NeuCLIR Track 2022, you can download collection from the link provided on [this page](https://trec.nist.gov/act_part/tracks2022.html). Note that you will need trec2022 credentials to download this file (provided when you register for TREC).**

This repository contains the scripts for downloading and validating scripts for the documents. 
Document ids are stored in `resource/{lang}/ids.*.jsonl.gz`

Required packages for the scripts are recorded in `requirements.txt`. 

We recommand creating a new python environment for downloading. Package versions could have some unintentional effect on decoding 
the documents from Common Crawl. Please contact us if you have documents with mismatch hashs. 

## Download Documents

To download the documents from Common Crawl, please use the following command.
If you plan to use this collection with [`ir_datasets`](https://ir-datasets.com/), please specify `~/.ir_datasets/neuclir/1` 
as the storage or make a soft link to to the directory you wish to store the documents. The document ids and hashs are 
stored in `resource/{lang}/ids.*.jsonl.gz`. 

```bash
python download_documents.py --storage ./data/ \
                             --zho ./resource/zho/ids.*.jsonl.gz \
                             --fas ./resource/fas/ids.*.jsonl.gz \
                             --rus ./resource/rus/ids.*.jsonl.gz \
                             --jobs 4 --check_hash
```

If you wish to only download the documents for one language, just specify the id file for the language
you wish to download. 
In case the URLs for the Common Crawl files change in the future, the flag `--cc_base_url` provides the options 
to specify an alternative URL for the files. The current default value points to `https://data.commoncrawl.org/`. 
The full description of the arguments can be found when execute with the `--help` flag.

## Postprocessing of the Downloaded Documents

Multiprocessing during download results in arbitrary ordering of the documents in the saved `.jsonl` files. 
To support full reproducibility, we provide script to postprocess the file to match the document order specified in the document id files. 
`fix_document_order.py` changes the ordering of the documents, validates the document hashs, and verifies all and only specified documents are in 
the result file. The unsorted file will be renamed as `docs.jsonl.bak`. You could delete the file manually. Following is a sample command. 

```bash
python fix_document_order.py --raw_download_file ./data/{lang}/docs.jsonl \
                             --id_file ./resource/{lang}/ids.*.jsonl.gz \
                             --check_hash
```

**If the script identifies missing files during postprocessing, please rerun the downloading script with `--resume` flag to get the missing documents.**
**Some files might be missing due to temporary network failure or connection refused by the Common Crawl servers.**
**Rerunning the downloading script usually would be able to retrieve those documents. If not, please raise issue with the document id to bring this to our attention.**

## Converting the Chinese Character Sets

The Chinese document collection contains both traditional and simplified Chinese characters from their original source. 
We provide a script for converting the documents to either traditional or simplified characters if the users would like 
to have an unified character set. The following command creates a `docs.{traditional, simplified}.jsonl` file in the 
same directory as the original `docs.jsonl` file. 

```bash
python convert_chinese_char.py --document_file ./data/zho/docs.jsonl \
                               --convert_to {traditional, simplified} \
                               --line_count 3179209
```
