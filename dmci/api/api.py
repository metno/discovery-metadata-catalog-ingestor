from flask import Flask, request
import numpy as np

def validate_mmd(data):
    return np.random.choice([True,False])
    
app = Flask(__name__)
@app.route('/', methods=['POST'])
def base():
    data = request.get_data()
    if validate_mmd(data):
        return "Nom nom I like this MMD file.", 200
    else:
        return "🤮", 500
    
app.run()

