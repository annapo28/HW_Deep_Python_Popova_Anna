import pathlib

def is_circular_symlink(symlink: pathlib.Path) -> bool:
    if not symlink.exists() and not symlink.is_symlink():
        raise FileNotFoundError(f"Path {symlink} does not exist.")

    if not symlink.is_symlink():
        raise RuntimeError(f"Path {symlink} is not a symbolic link.")

    visited = set()
    current_path = symlink

    while True:
        try:
            target = current_path.readlink()
        except RuntimeError:
            return False

        if target.is_absolute():
            target_path = target
        else:
            target_path = current_path.parent / target

        if target_path in visited:
            return True
        visited.add(target_path)

        if not target_path.exists() and not target_path.is_symlink():
            return False
        
        if not target_path.is_symlink():
            break

        current_path = target_path

    return False
