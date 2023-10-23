# BFGS.py

from ase.optimize import BFGS


def main(atoms_list, fmax=0.05):
    """Does BFGS relaxations of atoms

    Args:
        atoms_list (list): list of atoms, as given by runner
    Returns:
        atoms object: relaxed atoms object"""
    atoms = atoms_list[0]
    opt = BFGS(atoms)
    opt.run(fmax=fmax)

    return atoms
