# Install or update needed software
sudo apt-get update
sudo apt-get install -yq git supervisor python3 python3-dev python3-venv

sudo wget https://bootstrap.pypa.io/get-pip.py
sudo rm /usr/lib/python3.11/EXTERNALLY-MANAGED
sudo python3 get-pip.py

# Fetch source code
export HOME=/root
git clone https://github.com/alex-rsiqueira/binance-trade-bot.git /opt/app

# Install Cloud Ops Agent
sudo bash /opt/app/src/add-google-cloud-ops-agent-repo.sh --also-install

# Account to own server process
useradd -m -d /home/pythonapp pythonapp

# Python environment setup
cd /opt/app
sudo python3 -m venv env
source env/bin/activate
sudo /opt/app/env/bin/pip install -r /opt/app/src/requirements.txt

# Set ownership to newly created account
sudo chown -R pythonapp:pythonapp /opt/app

# Put supervisor configuration in proper place
sudo cp /opt/app/config/python-app.conf /etc/supervisor/conf.d/python-app.conf

# Start service via supervisorctl
supervisorctl reread
supervisorctl update