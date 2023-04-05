import hashlib
salt = 'somesalt'
split_rate = 0.5

def get_exp_group(user_id: int) -> str:
    hash_ = int(hashlib.md5((salt + repr(user_id)).encode()).hexdigest(),16)
    if hash_ % 2 == 0:
        return "test"
    else:
        return "control"