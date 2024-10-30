from abc import ABC, abstractmethod

class MessageHandler(ABC):
    @abstractmethod
    def handle_message(self, msg):
        """Phương thức này được triển khai bởi các lớp con."""
        pass
