import json
PATH_TO_ORIGINAL_FILE = "<tutaj ścieżka do pliku ze wszsytkimi danymi>"
LIMIT=600


if __name__ == '__main__':
    i=0
    with open(PATH_TO_ORIGINAL_FILE) as f, open("users_to_fetch.txt", "w") as out:
        for line in f:
            j = json.loads(line)
            out.write(j["login"])
            out.write("\n")
            if i < LIMIT:
                i += 1
            else:
                break
