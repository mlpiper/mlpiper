
def sink_decode(str):
    return ''.join(chr(ord(letter) - 1) for letter in str)