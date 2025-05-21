#####################################################
# Packages                                          #
#####################################################


#####################################################
# Class                                             #
#####################################################

class JsUtil:

    """
    A utility class for handling dict/json operations.
    """

    @staticmethod
    def drill_down_dict(p_object: dict, p_nested_keys: list, strict = True) -> None:

        """
        Traverses a nested dictionary using a list of keys and returns the corresponding value.
        """

        # Avoid modifying the original dictionary
        object_copy = p_object.copy()
        
        for nested_key in p_nested_keys:

            if strict:
                object_copy = object_copy[nested_key]
            else:
                if nested_key in object_copy:
                    object_copy = object_copy.get(nested_key)
                else:
                    return None

        return object_copy