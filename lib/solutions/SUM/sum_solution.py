
class SumSolution:
    
    def compute(self, x: int, y: int) -> int:
        """
            Calculates the sum of two numbers.

            Args:
                x (int): The first number to add. 0-100
                y (int): The second number to add. 0-100

            Returns:
                int: The sum of x and y.

            Raises:
                TypeError: If either x or y is not an integer.
        """
        if not isinstance(x, int) or not isinstance(y, int):
            raise TypeError(f"Both arguments x:{x} and y:{y} must be integers")
        if not (0 <= x <= 100) or not (0 <= y <= 100):
            raise ValueError(f"Both arguments x:{x} and y:{y} must be between 0 and 100")
        return x + y



