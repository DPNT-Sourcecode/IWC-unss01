
class HelloSolution:
    
    # friend_name = unicode string
    def hello(self, friend_name: str) -> str:
        """
            Returns a greeting message for a given friend name.

            Args:
                friend_name (str): The name of the friend to greet.

            Returns:
                str: A greeting message for the friend.
        """
        if not isinstance(friend_name, str):
            raise TypeError(f"friend_name must be a string, got {type(friend_name)}")
        return f"Hello, {friend_name}!"