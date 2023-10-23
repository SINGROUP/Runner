# interaction_energy.py


def main(atoms_list, d=1):
    """Combines atoms with a distance d

    Args:
        atoms_list (list): list of atoms, as given by runner

    Returns:
        atoms object: combined atoms with calculated energies"""

    atoms0 = atoms_list[1]

    atoms0.positions[:, 2] += d + 1

    atoms0 += atoms_list[2]

    # making sure new energies are used in the system with updated atoms
    atoms0.get_potential_energy()

    # adding key_value_pairs for column search in database
    # these are updated to the old key_value_pairs already in database
    atoms0.info["key_value_pairs"] = {"d": d}

    return atoms0
