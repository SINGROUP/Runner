"""
python file to run python tasks
"""

import argparse
import json
import pickle


def main():
    # parse args
    parser = argparse.ArgumentParser(description="Run function with params.")
    parser.add_argument("func", type=str)
    parser.add_argument("indx", type=int)
    args = parser.parse_args()

    # import module
    func = __import__(args.func)

    # open params and atoms
    with open(f"params{args.indx}.json") as fio:
        params = json.load(fio)
    with open("atoms.pkl", "rb") as fio:
        atoms = pickle.load(fio)

    # run func
    atoms = func.main(atoms, **params)

    # write atoms
    with open("atoms.pkl", "wb") as fio:
        pickle.dump(atoms, fio)


if __name__ == "__main__":
    main()
