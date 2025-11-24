#!/bin/bash

source venv/bin/activate

python3 mock-client.py \
	--operation demo \
	--base-url https://cai-router-bss.spitch.ai/mock \
	--api-key test_12345 \
	--files demo_assets/call1.wav \
	demo_assets/call2.mp3 \
	demo_assets/MZceedc1ae6a69b11dba917b04f0fb0435.mp3 \
	demo_assets/MZceedc1ae6a69b11dba917b04f0fb0433.mp3 \
	--meta demo_assets/call1.json \
	demo_assets/call2.json \
	demo_assets/MZceedc1ae6a69b11dba917b04f0fb0435.json \
	demo_assets/MZceedc1ae6a69b11dba917b04f0fb0433.json
