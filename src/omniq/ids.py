import ulid

def new_ulid() -> str:
    return str(ulid.new())
