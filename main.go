package main

import (
	"fmt"
	"os"
	"strconv"
)

// print_partition_info displays the length of each partition in order
func print_partition_info(part [][]int) {
	fmt.Println("Num partions = ", len(part))
	for i, v := range part {
		fmt.Printf("size of part %d = %d\n", i, len(v))
	}
}

// generate_data creates a dataset with a value per int in a range from 0 to size
func generate_data(size int) []int {
	data := make([]int, 0)
	for i := 0; i < size; i++ {
		data = append(data, i)
	}
	return data
}

// part_in_two seperates a dataset in the middle into two slices
func part_in_two(part []int) [][]int {
	part_size := len(part) / 2
	split := make([][]int, 2)

	for i := 0; i < part_size; i++ {
		split[0] = append(split[0], part[i])
	}

	for j := part_size; j < len(part); j++ {
		split[1] = append(split[1], part[j])
	}

	return split
}

// map_data calculates the sum of a partition
func map_data(data []int) int {
	sum := 0
	for _, v := range data {
		sum += v
	}
	return sum
}

// partition_data seperates a dataset into num_part partitions
func partition_data(num_part int, data []int) [][]int {
	parted := make([][]int, num_part)
	part_pos := 0
	for i := 0; i < len(data); i++ {
		if part_pos == num_part {
			part_pos = 0
		}
		parted[part_pos] = append(parted[part_pos], data[i])
		part_pos += 1
	}
	return parted
}

// wrap_map_data creates a goroutine to map_data for a partition
func wrap_map_data(data []int) chan int {
	res := make(chan int)
	go func() {
		defer close(res)
		res <- map_data(data)
	}()
	return res
}

// main parses cli ali args and manages map reduce for a data set into two
// and then into num partitions passed in by cli
func main() {
	if len(os.Args[1:]) != 2 {
		fmt.Println("Invalid number of args, exiting!")
		os.Exit(1)
	}

	num_partitions, _ := strconv.Atoi(os.Args[1])
	num_data, _ := strconv.Atoi(os.Args[2])

	if num_data < num_partitions {
		fmt.Println("Partitions can not be greater than the size of data")
		os.Exit(1)
	}

	if num_partitions < 1 {
		panic("partitions must be atleast 1")
	}

	data := generate_data(num_data)
	part_two := part_in_two(data)
	print_partition_info(part_two)

	intermediate_two := make([]int, 2)

	intermediate_two[0] = <-wrap_map_data(part_two[0])
	intermediate_two[1] = <-wrap_map_data(part_two[1])

	fmt.Println("intermediate sums for part two = ", intermediate_two)

	fmt.Println("Final sum for part two = ", map_data(intermediate_two))

	part_full := partition_data(num_partitions, data)
	print_partition_info(part_full)
	intermediate_full := make([]int, num_partitions)

	for i := 0; i < num_partitions; i++ {
		intermediate_full[i] = <-wrap_map_data(part_full[i])
	}

	fmt.Println("Intermediate sums full = ", intermediate_full)
	fmt.Println("Sum full = ", map_data(intermediate_full))
}
