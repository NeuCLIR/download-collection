import pandas as pd
import gzip, json
from pathlib import Path

# hard-coded a lot of things with the github action

ndocs = {'fas': 2232016, 'rus': 4627543, 'zho': 3179209}

def main():
    logs = list(Path('./data/logs').glob("download_log.*.txt"))
    mismatches = {'zho':[], 'fas':[], 'rus':[]}
    for fn in logs:
        for l in fn.open():
            if "hash-mismatch" in l:
                l = l.split("record-id: ")[1].strip().split()
                mismatches[ l[1].replace(',', '') ].append({
                    'id': l[0].replace(',', ''), 
                    'expect': l[3],
                    'got': l[6]
                })
    
    print("# Mismatch Hashs")
    for lang in mismatches:
        print(f"### {lang} ({len(mismatches[lang])}/{ndocs[lang]} -- {len(mismatches[lang])/ndocs[lang]*100:.4f}%)")
        if len(mismatches[lang]) > 200:
            print("More than 200 mismatches found -- see artifacts for complete list.")
            print(pd.DataFrame(mismatches[lang]).iloc[:200].to_markdown(index=False))
        else:
            print(pd.DataFrame(mismatches[lang]).to_markdown(index=False))
    
    with gzip.open('report.jsonl.gz', 'wt') as fw:
        for lang in mismatches:
            for l in mismatches[lang]:
                fw.write(json.dumps({'lang': lang, **l}))

if __name__ == '__main__':
    main()