from bs4 import BeautifulSoup
import re
import pandas as pd
import glob

def getfaz(path):
    with open(path, 'rb') as f:
        soup = BeautifulSoup(f.read(), 'html')
    time, title, meta, text = [], [], [], []

    for e in soup.find_all("div", class_="single-document"):
        meta0 = [ele.text for ele in e.find_all("pre") if ele['class'][0] not in ["text", "docImage", "docCopy"]]
        meta.append("\n".join(meta0))
        t = re.findall(r'\d{2}.\d{2}.\d{4}', e.find("pre", class_="docSource").text)[0]
        time.append(re.sub(r'(\d{2}).(\d{2}).(\d{4})', r'\3-\2-\1', t))
        title.append(e.find("pre", class_="docTitle").text)
        text.append(e.find("pre", class_="text").text)

    return pd.DataFrame({'title': title, 'time': time, 'text': text, 'meta': meta})

def getendf(path):
    with open(path, 'rb') as f:
        soup = BeautifulSoup(f.read(), 'html')

    dictde = {"Januar": "01", "Februar": "02", "März": "03", "April": "04", "Mai": "05", "Juni": "06",
              "Juli": "07", "August": "08", "September": "09", "Oktober": "10", "November": "11", "Dezember": "12"}

    time, title, author, meta, text, terms, ids = [], [], [], [], [], [], []

    for e in soup.find_all(class_="article enArticle"):
        title.append(e.find("div", id="hd").text)
        ids.append(e.find_all("p")[-1].get_text())
        meta0 = "\n".join(t.text for t in e.find_all("div"))
        meta.append(meta0)
        t = re.findall(r'(\d{1,2})\s(\w+)\s(\d{4})', meta0)[0]
        d = f"{int(t[0]):02d}"
        m = dictde[t[1]]
        time.append(f"{t[2]}-{m}-{d}")
        text.append("\n".join(t.text for t in e.find_all(class_="articleParagraph enarticleParagraph")))
        terms.append([t.text for t in e.find_all("b")])
        author.append(e.find("div", class_="author").text if e.find("div", class_="author") else "")

    return pd.DataFrame({'title': title, 'time': time, 'author': author, 'text': text, 'terms': terms, 'meta': meta, 'id': ids})

def getdedf(path):
    with open(path, 'rb') as f:
        soup = BeautifulSoup(f.read(), 'html')

    dictde = {"Januar": "01", "Februar": "02", "März": "03", "April": "04", "Mai": "05", "Juni": "06",
              "Juli": "07", "August": "08", "September": "09", "Oktober": "10", "November": "11", "Dezember": "12"}

    time, title, author, meta, text, terms, ids = [], [], [], [], [], [], []

    for e in soup.find_all(class_="article deArticle"):
        title.append(e.find("div", id="hd").text)
        ids.append(e.find_all("p")[-1].get_text())
        meta0 = "\n".join(t.text for t in e.find_all("div"))
        meta.append(meta0)
        t = re.findall(r'(\d{1,2})\s(\w+)\s(\d{4})', meta0)[0]
        d = f"{int(t[0]):02d}"
        m = dictde[t[1]]
        time.append(f"{t[2]}-{m}-{d}")
        text.append("\n".join(t.text for t in e.find_all(class_="articleParagraph dearticleParagraph")))
        terms.append([t.text for t in e.find_all("b")])
        author.append(e.find("div", class_="author").text if e.find("div", class_="author") else "")

    return pd.DataFrame({'title': title, 'time': time, 'author': author, 'text': text, 'terms': terms, 'meta': meta, 'id': ids})

colfinal = ["time", "title", "text", "meta", "media", "terms", "id"]
colfinal2 = ["time", "title", "text", "meta", "media", "terms"]

# Load keywords
keywords_df = pd.read_csv('/Users/xixuanzhang/Documents/Neovex/Datenerhebung/reddit/keywords_complete_deduplicated.csv', encoding="utf-8")
keylist = keywords_df["keyword_ger"].tolist()
keylist += [k.replace("-", " ") for k in keylist if "-" in k]
regex = re.compile(r'\b(' + "|".join(set(keylist)) + r')\b', re.IGNORECASE)

# Parse FAZ
faz = pd.DataFrame()
for file in glob.glob("/Users/xixuanzhang/Desktop/GER_Media/FAZ/*"):
    faz = pd.concat([faz, getfaz(file)], axis=0)
faz = faz.reset_index(drop=True)
faz["text1"] = faz["title"] + "\n" + faz["text"].str.replace(r"https?://\S+", "", regex=True)
faz = faz.drop_duplicates("text1")
faz = faz[faz["text1"].str.contains(regex)]
faz["media"] = "faz"
faz["terms"] = faz["text"].str.findall(regex)
faz = faz[colfinal2]

# Parse US media
us = pd.DataFrame()
uslist = ["New York Times", "USA Today", "Wall Street Journal", "Washington Post"]
for media in uslist:
    for file in glob.glob(f"/Users/xixuanzhang/Desktop/US_Media/{media}/*"):
        df = getendf(file)
        df["media"] = media
        us = pd.concat([us, df], axis=0)
us = us.drop_duplicates("id").reset_index(drop=True)
us["textonly"] = us["text"]
us["title"] += "\n"
us["text"] = us["title"] + us["textonly"]
us["terms"] = us["text"].str.findall(regex)
us = us[us["terms"].map(len) > 0]
us = us[colfinal]

# Parse German media
ger = pd.DataFrame()
gerlist = ["Bild", "SZ", "taz", "Welt"]
for media in gerlist:
    for file in glob.glob(f"/Users/xixuanzhang/Desktop/GER_Media/{media}/*"):
        df = getdedf(file)
        df["media"] = media
        ger = pd.concat([ger, df], axis=0)
ger = ger.drop_duplicates("id").reset_index(drop=True)
ger["textonly"] = ger["text"]
ger["title"] += "\n"
ger["text"] = ger["title"] + ger["textonly"]
ger["terms"] = ger["text"].str.findall(regex)
ger = ger[ger["terms"].map(len) > 0]
ger = pd.concat([faz, ger], axis=0)[colfinal]

# ger.to_pickle("/path/to/ger_output.pkl")
# us.to_pickle("/path/to/us_output.pkl")
