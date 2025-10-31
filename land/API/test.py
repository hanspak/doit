from flask import Flask, render_template_string

app = Flask(__name__)

@app.route('/')  
def home():
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>샘플 페이지</title>
    </head>
    <body>
        <h1>안녕하세요! 파이썬 HTML 샘플입니다.</h1>
        <p>이 페이지는 Flask로 만들어졌습니다.</p>
    </body>
    </html>
    """
    return render_template_string(html)

if __name__ == '__main__':
    app.run(debug=True)