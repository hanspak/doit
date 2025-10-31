from kivy.app import App
from kivy.uix.label import Label
from kivy.clock import Clock
from datetime import datetime

class TimeLabel(Label):
    def update_time(self, *args):
        self.text = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

class TimeApp(App):
    def build(self):
        self.label = TimeLabel(font_size=40)
        Clock.schedule_interval(self.label.update_time, 1)  # 매 1초마다 갱신
        return self.label

if __name__ == '__main__':
    TimeApp().run()