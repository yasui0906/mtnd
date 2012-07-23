for i in $(seq 1 $CDATA);do
  ${srcdir}/mtnfile -P $WBASE/data/data$i ${REMOTEPATH[$i]}/data$i
  md1=$(cat $WBASE/data/data$i | md5sum)
  md2=$(cat $WBASE/export/${REMOTEPATH[$i]}/data$i | md5sum)
  assert_cmp "$md1" "$md2"
done
