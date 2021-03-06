#!/bin/bash

for bs in 0 1 2 1024 1025 $((128*1024-1)) $((128*1024)) $((128*1024+1)) 1m 1234567 20m
do
    echo "====== Trying block size of $bs ======"
    dd if=/dev/random of=$bs bs=$bs count=1 2>/dev/null

    ./test-hdo $bs $bs.hz.gz 2>&1 | grep hash > tmp.txt

    if [ ! -s "$bs.hz.gz" ]; then
        echo "test-hdo broke and returned a zero-sized file" >&2
        rm "$bs.hz.gz" $bs
        continue
    fi

    result_comp=$(awk '/^Saved hash:/ { print $3 }' tmp.txt)
    result_uncomp=$(awk '/^Original hash:/ { print $3 }' tmp.txt)
    actual_comp=$(shasum $bs.hz.gz | awk '{print $1}')
    actual_uncomp=$(gunzip -c $bs.hz.gz | shasum | awk '{print $1}')

    original_uncomp=$(shasum $bs | awk '{print $1}')
    rm $bs $bs.hz.gz tmp.txt

    if [ "$result_comp" == "$actual_comp" ]; then
        echo "SHA1 for compressed stream is calculated correctly ($result_comp)"
    else
        echo "SHA1 for compressed stream does NOT match" >&2
        echo "$result_comp != $actual_comp" >&2
    fi

    if [ "$result_uncomp" == "$actual_uncomp" ]; then
        echo "SHA1 for uncompressed stream is calculated correctly ($result_uncomp)"
    else
        echo "SHA1 for uncompressed stream does NOT match calculated value" >&2
        echo "$result_comp != $actual_comp" >&2
    fi

    if [ "$actual_uncomp" == "$original_uncomp" ]; then
        echo "SHA1 for decompressed stream matches original input ($actual_uncomp)"
    else
        echo "SHA1 for decompressed stream does NOT match input" >&2
        echo "$actual_uncomp != $original_uncomp" >&2
    fi
done
