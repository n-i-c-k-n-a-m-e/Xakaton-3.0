import requests
for i in range(0,24):
    for j in range(0, 6):
        try:
            if i < 9:
                url = f'https://raw.githubusercontent.com/d-yacenko/dataset/refs/heads/main/telecom10k/psx_62.0_2024-01-01%200{i}%3A{j}0%3A00.csv'
            else:
                url = f'https://raw.githubusercontent.com/d-yacenko/dataset/refs/heads/main/telecom10k/psx_62.0_2024-01-01%20{i}%3A{j}0%3A00.csv'
            r = requests.get(url)
            if 'Not' in r.text:
                continue
        except:
            continue
        with open(f'datasets/psx_62.0_2024-01-01_{i}-00-00.csv', 'a') as f:
            f.write(r.text)