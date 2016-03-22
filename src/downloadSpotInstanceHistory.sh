#!/bin/bash

region="$1"
instanct_types=("g2.2xlarge" "g2.8xlarge")

function get-availability-zone {
  ec2-describe-availability-zones --region $1 | awk '{print $2}'
}

function get-regions {
  ec2-describe-regions | awk '{print $2}'
}

regions=$(get-regions)
for r in $regions
do
	echo "$r"
	available_zones=$(get-availability-zone "$r")
	for az in $available_zones
	do
		echo $az
		for it in ${instanct_types[@]}
		do	
			echo $it
				ec2-describe-spot-price-history  --start-time 2014-03-16T15:00:00 -a "$az" --region "$r" -t "$it"  -d Linux/UNIX |  awk '{print $2"\t"$3}' > "$az"_"$it"_linux

		done

	done
done
