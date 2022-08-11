import pandas as pd
from pathlib import Path

# hard-coded a lot of things with the github action

ndocs = {'fas': 2232016, 'rus': 4627543, 'zho': 3179209}

def main():
    logs = list(Path('./data').glob("download_log.*.txt"))
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
        print(f"## {lang} ({len(mismatches[lang])}/{ndocs[lang]} -- {len(mismatches[lang])/ndocs[lang]*100:.4f}%)")
        print(pd.DataFrame(mismatches[lang]).to_markdown(index=False))

if __name__ == '__main__':
    main()