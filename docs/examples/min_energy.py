# min_energy.py


def main(atoms_list):
    """Returns atoms with the lowest energy

    Args:
        atoms_list (list): list of atoms, as given by runner

    Returns:
        atoms object: atoms with lowest calculated energies"""
    import numpy as np

    en_list = [atoms.get_potential_energy() for atoms in atoms_list[1:]]

    indx = np.argmin(en_list)

    return atoms_list[indx + 1]
