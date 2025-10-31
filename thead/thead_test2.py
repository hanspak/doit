import time
import threading

def long_task():
	for i in range(5):
		time.sleep(1)
		print("working:%s\n" %i)

print("start")


threads = []

for i in range(5):
	t=threading.Thread(target=long_task)
        threads.append(t)

for t  in threads :
	t.start()


print("end")
