for i in $(seq 1 $CDATA);do
  md1=$(${srcdir}/mtnfile -G - ${REMOTEPATH[$i]}/data$i | md5sum)
  md2=$(cat $WBASE/export/${REMOTEPATH[$i]}/data$i      | md5sum)
  assert_cmp "$md1" "$md2"
done
