#!/bin/bash

PERCENT='0.5';
rm -f list.txt;
pga list -f json > list.tmp.txt;
cnt=$(cat list.tmp.txt | wc -l);
target=$(bc <<< "scale=0;${cnt}*${PERCENT}/100" | cut -d'.' -f 1);
cat list.tmp.txt | sort -R | head "-${target}" | jq -r '.sivaFilenames[]' > list.txt;
rm list.tmp.txt;
echo "Generated a dataset selecting ${target} random repositories out of ${cnt} (${PERCENT}).";