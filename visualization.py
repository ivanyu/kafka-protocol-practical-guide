def visualize_byte_array(array: bytes) -> None:
    for i in array:
        print("{:02x}".format(i), end=" ")


def visualize_varint(array: bytes) -> str:
    result = ""
    for b in array:
        result += format_byte(b) + " "
    return result


def format_byte(b: int) -> str:
    result = "{:08b}".format(b)
    result = "[" + result[0] + "|" + result[1:] + "]"
    return result
