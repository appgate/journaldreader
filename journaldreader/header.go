package main

import (
	"github.com/edsrzf/mmap-go"
	"os"
	"unsafe"
	"fmt"
)

// _packed_ struct Header {
//         uint8_t signature[8]; /* "LPKSHHRH" */
//         le32_t compatible_flags;
//         le32_t incompatible_flags;
//         uint8_t state;
//         uint8_t reserved[7];
//         sd_id128_t file_id;
//         sd_id128_t machine_id;
//         sd_id128_t tail_entry_boot_id;
//         sd_id128_t seqnum_id;
//         le64_t header_size;
//         le64_t arena_size;
//         le64_t data_hash_table_offset;
//         le64_t data_hash_table_size;
//         le64_t field_hash_table_offset;
//         le64_t field_hash_table_size;
//         le64_t tail_object_offset;
//         le64_t n_objects;
//         le64_t n_entries;
//         le64_t tail_entry_seqnum;
//         le64_t head_entry_seqnum;
//         le64_t entry_array_offset;
//         le64_t head_entry_realtime;
//         le64_t tail_entry_realtime;
//         le64_t tail_entry_monotonic;
//         /* Added in 187 */
//         le64_t n_data;
//         le64_t n_fields;
//         /* Added in 189 */
//         le64_t n_tags;
//         le64_t n_entry_arrays;
//         /* Added in 246 */
//         le64_t data_hash_chain_depth;
//         le64_t field_hash_chain_depth;
//         /* Added in 252 */
//         le32_t tail_entry_array_offset;
//         le32_t tail_entry_array_n_entries;
//         /* Added in 254 */
//         le64_t tail_entry_offset;
// };

type Header struct {
	signature [8]byte
}


func main() {

	f, err := os.OpenFile("system.journal", os.O_RDONLY, 0)
	if err != nil {
		//TODO
	}

	m, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		//TODO
	}

	// TODO
	// defer f.Close()
	// if err := m.Unmap(); err != nil {

	h := (*Header)(unsafe.Pointer(&m[0]))
	if string(h.signature[:]) != "LPKSHHRH" {
		//TODO
	}



}
