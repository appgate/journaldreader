/* SPDX-License-Identifier: LGPL-2.1-or-later */

/*
 * This file is based on journal-def.h in systemd.
 * The constants and structs were obtained from there and converted to
 * go.
 *
 * The code to operate on the data structures is original.
 *
 * Copyright for the original file:
 *
 * 2008-2015 Kay Sievers <kay@vrfy.org>
 * 2010-2015 Lennart Poettering
 * 2012-2015 Zbigniew Jędrzejewski-Szmek <zbyszek@in.waw.pl>
 * 2013-2015 Tom Gundersen <teg@jklm.no>
 * 2013-2015 Daniel Mack
 * 2010-2015 Harald Hoyer
 * 2013-2015 David Herrmann
 * 2013, 2014 Thomas H.P. Andersen
 * 2013, 2014 Daniel Buch
 * 2014 Susant Sahani
 * 2009-2015 Intel Corporation
 * 2000, 2005 Red Hat, Inc.
 * 2009 Alan Jenkins <alan-jenkins@tuffmail.co.uk>
 * 2010 ProFUSION embedded systems
 * 2010 Maarten Lankhorst
 * 1995-2004 Miquel van Smoorenburg
 * 1999 Tom Tromey
 * 2011 Michal Schmidt
 * 2012 B. Poettering
 * 2012 Holger Hans Peter Freyther
 * 2012 Dan Walsh
 * 2012 Roberto Sassu
 * 2013 David Strauss
 * 2013 Marius Vollmer
 * 2013 Jan Janssen
 * 2013 Simon Peeters
 *
 * Copyright for the go version:
 *
 * 2024 Appgate Inc.
 *
 * Date: 31-10-2024
 */
package journaldreader

import (
	"encoding/binary"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/klauspost/compress/zstd"
	"os"
	"unsafe"
)

const HEADER_SIZE = 208            //struct.calcsize('<8s 2I B 7x 16s 16s 16s 16s 15Q')
const OBJECT_HEADER_SIZE = 16      //struct.calcsize('<2B 6x Q')
const ENTRY_ARRAY_OBJECT_SIZE = 24 //OBJECT_HEADER_SIZE + struct.calcsize('<2B 6x Q Q')
const ENTRY_OBJECT_SIZE = 64       //OBJECT_HEADER_SIZE + struct.calcsize('<3Q 16s Q')
const DATA_OBJECT_SIZE = 64        //OBJECT_HEADER_SIZE + struct.calcsize('<6Q')

const OBJECT_UNUSED = 0 // also serves as "any type" or "additional category"
const OBJECT_DATA = 1
const OBJECT_FIELD = 2
const OBJECT_ENTRY = 3
const OBJECT_DATA_HASH_TABLE = 4
const OBJECT_FIELD_HASH_TABLE = 5
const OBJECT_ENTRY_ARRAY = 6
const OBJECT_TAG = 7
const _OBJECT_TYPE_MAX = 8
const _OBJECT_TYPE_INVALID = -22 // -EINVAL

const OBJECT_COMPRESSED_XZ = 1 << 0
const OBJECT_COMPRESSED_LZ4 = 1 << 1
const OBJECT_COMPRESSED_ZSTD = 1 << 2
const _OBJECT_COMPRESSED_MASK = OBJECT_COMPRESSED_XZ | OBJECT_COMPRESSED_LZ4 | OBJECT_COMPRESSED_ZSTD

const HEADER_INCOMPATIBLE_COMPRESSED_XZ = 1 << 0
const HEADER_INCOMPATIBLE_COMPRESSED_LZ4 = 1 << 1
const HEADER_INCOMPATIBLE_KEYED_HASH = 1 << 2
const HEADER_INCOMPATIBLE_COMPRESSED_ZSTD = 1 << 3
const HEADER_INCOMPATIBLE_COMPACT = 1 << 4

type Header struct {
	signature               [8]byte
	compatible_flags        uint32
	incompatible_flags      uint32
	state                   uint8
	__padding               [7]byte
	file_id                 [16]byte
	machine_id              [16]byte
	tail_entry_boot_id      [16]byte
	seqnum_id               [16]byte
	header_size             uint64
	arena_size              uint64
	data_hash_table_offset  uint64
	data_hash_table_size    uint64
	field_hash_table_offset uint64
	field_hash_table_size   uint64
	tail_object_offset      uint64
	n_objects               uint64
	n_entries               uint64
	tail_entry_seqnum       uint64
	head_entry_seqnum       uint64
	entry_array_offset      uint64
	head_entry_realtime     uint64
	tail_entry_realtime     uint64
	tail_entry_monotonic    uint64
}

type ObjectHeader struct {
	type_     uint8
	flags     uint8
	__padding [6]byte
	size      uint64
}

type EntryArrayObject struct {
	object                  ObjectHeader
	next_entry_array_offset uint64
}

func (j *SdjournalReader) _loadEntryArrayObject(offset uint64) error {

	if (offset & 7) != 0 {
		return fmt.Errorf("Unaligned offset")
	}

	if uint64(len(j.data))-offset < ENTRY_ARRAY_OBJECT_SIZE {
		return fmt.Errorf("EOF")
	}

	h := (*EntryArrayObject)(unsafe.Pointer(&j.data[offset]))

	if h.object.type_ != OBJECT_ENTRY_ARRAY {
		return fmt.Errorf("Unexpected object encountered at %d", offset)
	}

	j.array_iterator = 0
	j.entry_array_offset = offset
	j.entryarray = h

	return nil
}

func (j *SdjournalReader) _next_entry_offset() (uint64, error) {
	compact := (j.header.incompatible_flags & HEADER_INCOMPATIBLE_COMPACT) != 0

	realsize := j.entryarray.object.size - ENTRY_ARRAY_OBJECT_SIZE

	var item_size uint64
	if compact {
		item_size = 32 / 8
	} else {
		item_size = 64 / 8
	}

	array_size := realsize / item_size

	if j.array_iterator < array_size {
		slice := j.data[j.entry_array_offset+ENTRY_ARRAY_OBJECT_SIZE+(item_size*j.array_iterator) : j.entry_array_offset+ENTRY_ARRAY_OBJECT_SIZE+(item_size*j.array_iterator)+item_size]

		var entry_offset uint64

		if compact {
			entry_offset = uint64(binary.LittleEndian.Uint32(slice))
		} else {
			entry_offset = binary.LittleEndian.Uint64(slice)
		}

		j.array_iterator++
		return entry_offset, nil
	} else {
		if j.entryarray.next_entry_array_offset == 0 {
			return 0, fmt.Errorf("No more items")
		}
		err := j._loadEntryArrayObject(j.entryarray.next_entry_array_offset)
		if err != nil {
			return 0, err
		}
		return j._next_entry_offset()
	}

	return 0, fmt.Errorf("Unreacheable")
}

type EntryObject struct {
	object    ObjectHeader
	seqnum    uint64
	realtime  uint64
	monotonic uint64
	boot_id   [16]byte
	xor_hash  uint64
}

func (j *SdjournalReader) _loadDataOffsetsFromEntry(offset uint64) ([]uint64, error) {
	if (offset & 7) != 0 {
		return nil, fmt.Errorf("Unaligned offset")
	}

	if uint64(len(j.data))-offset < ENTRY_OBJECT_SIZE {
		return nil, fmt.Errorf("EOF")
	}

	h := (*EntryObject)(unsafe.Pointer(&j.data[offset]))

	if h.object.type_ != OBJECT_ENTRY {
		return nil, fmt.Errorf("Unexpected object encountered at %d", offset)
	}

	compact := (j.header.incompatible_flags & HEADER_INCOMPATIBLE_COMPACT) != 0

	realsize := h.object.size - ENTRY_OBJECT_SIZE

	var item_size uint64
	if compact {
		item_size = 4
	} else {
		item_size = 16
	}

	array_size := realsize / item_size

	r := make([]uint64, array_size)

	for i := uint64(0); i < array_size; i++ {

		slice := j.data[offset+ENTRY_OBJECT_SIZE+(item_size*i) : offset+ENTRY_OBJECT_SIZE+(item_size*i)+item_size]

		var data_offset uint64

		if compact {
			data_offset = uint64(binary.LittleEndian.Uint32(slice))
		} else {
			data_offset = binary.LittleEndian.Uint64(slice[0:8])
		}
		r[i] = data_offset
	}
	return r, nil
}

type DataObject struct {
	object             ObjectHeader
	hash               uint64
	next_hash_offset   uint64
	next_field_offset  uint64
	entry_offset       uint64
	entry_array_offset uint64
	n_entries          uint64
}

func (j *SdjournalReader) _loadData(offset uint64) ([]byte, error) {
	if (offset & 7) != 0 {
		return nil, fmt.Errorf("Unaligned offset")
	}

	if uint64(len(j.data))-offset < DATA_OBJECT_SIZE {
		return nil, fmt.Errorf("EOF")
	}

	h := (*DataObject)(unsafe.Pointer(&j.data[offset]))

	if h.object.type_ != OBJECT_DATA {
		return nil, fmt.Errorf("Unexpected object encountered at %d", offset)
	}

	compact := (j.header.incompatible_flags & HEADER_INCOMPATIBLE_COMPACT) != 0

	skip := uint64(0)
	if compact {
		// NOTE: Ignoring the maybe fields for simplicity
		skip = 8
	}

	realsize := h.object.size - DATA_OBJECT_SIZE - skip

	payload := j.data[offset+DATA_OBJECT_SIZE+skip : offset+DATA_OBJECT_SIZE+skip+realsize]

	if h.object.flags&OBJECT_COMPRESSED_XZ != 0 {
		return nil, fmt.Errorf("XZ decompression not implemented")
	} else if h.object.flags&OBJECT_COMPRESSED_LZ4 != 0 {
		return nil, fmt.Errorf("LZ4 decompression not implemented")
	} else if h.object.flags&OBJECT_COMPRESSED_ZSTD != 0 {
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
		if err != nil {
			return nil, err
		}
		return decoder.DecodeAll(payload, nil)
	}

	return payload, nil
}

type SdjournalReader struct {
	fd   *os.File
	data mmap.MMap

	header *Header

	entryarray         *EntryArrayObject
	entry_array_offset uint64
	array_iterator     uint64

	// Prevent reusing the object and doing anything before opening
	opened bool
	closed bool
}

func (j *SdjournalReader) Open(journalfile string) error {
	if j.opened {
		return fmt.Errorf("This object has been opened already")
	}
	if j.closed {
		return fmt.Errorf("This object has been closed already")
	}

	j.opened = true

	fd, err := os.OpenFile(journalfile, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	j.fd = fd

	data, err := mmap.Map(fd, mmap.RDONLY, 0)
	if err != nil {
		return err
	}
	j.data = data

	if len(data) < HEADER_SIZE {
		return fmt.Errorf("File is too small to read the header")
	}

	h := (*Header)(unsafe.Pointer(&data[0]))
	if unsafe.Sizeof(*h) != HEADER_SIZE {
		//NOTE There's no assertions in go, so we do it at runtime instead of compile time
		return fmt.Errorf("Unsupported architecture")
	}

	if string(h.signature[:]) != "LPKSHHRH" {
		return fmt.Errorf("Not a journal file")
	}

	j.header = h

	// Populate the initial array object
	err = j._loadEntryArrayObject(h.entry_array_offset)
	if err != nil {
		return err
	}

	return nil
}

func (j *SdjournalReader) Close() error {
	if !j.opened {
		return fmt.Errorf("This object hasn't been opened")
	}
	if j.closed {
		return fmt.Errorf("This object has been closed already")
	}

	j.closed = true
	j.opened = false

	err := j.data.Unmap()
	if err != nil {
		return err
	}
	j.fd.Close()

	return nil
}

func main() {
	j := SdjournalReader{}
	err := j.Open(os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	for true {
		offset, err := j._next_entry_offset()
		if err != nil || offset == uint64(0) {
			break
		}
		offsetdata, err := j._loadDataOffsetsFromEntry(offset)
		if err != nil {
			break
		}

		for i := 0; i < len(offsetdata); i++ {
			buf, err := j._loadData(offsetdata[i])
			if err != nil {
				fmt.Println(err)
				break
			}
			fmt.Println(string(buf))
		}

		fmt.Println("=========================")

	}

	j.Close()
}
