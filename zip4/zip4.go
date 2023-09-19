package usps

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/yeka/zip"
)

const zip4RecordLength = 182

const (
	Zip4CopyrightDetailCodeCopyright = "C"
	Zip4CopyrightDetailCodeDetail    = "D"
)

type Zip4Detail struct {
	ZipCode           string
	RecordTypeCode    string
	StateAbbreviation string
	CountyNumber      string
	Plus4LowNumber    Zip4Number
	Plus4HighNumber   Zip4Number
}

type Zip4Number string

func (n Zip4Number) Sector() string {
	return string(n[0:2])
}

func (n Zip4Number) Segment() string {
	return string(n[2:4])
}

func (n Zip4Number) IsDeliverable() bool {
	return n.Segment() != "ND"
}

func ReadCityStateFromZip4Tar(tarName string, zipPassword string, yield func(CityStateDetail)) error {
	bz, err := readTarEntry(tarName, "epf-zip4natl/ctystate/ctystate.zip")
	if err != nil {
		return err
	}

	zr, err := zip.NewReader(bytes.NewReader(bz), int64(len(bz)))
	if err != nil {
		return err
	}
	if len(zr.File) != 2 {
		return fmt.Errorf("expected 2 files in city state zip (found %v)", len(zr.File))
	}

	f := zr.File[0]
	if f.Name != "ctystate.txt" {
		return fmt.Errorf("unexpected file in city state zip: %v", f.Name)
	}

	f.SetPassword(zipPassword)
	r, err := f.Open()
	if err != nil {
		return err
	}
	defer r.Close()

	return ReadCityStateFile(r, yield)
}

func ReadZip4FromZip4Tar(tarName string, zipPassword string, yield func(Zip4Detail)) error {
	bz, err := readTarEntry(tarName, "epf-zip4natl/zip4/zip4.zip")
	if err != nil {
		return err
	}

	zr, err := zip.NewReader(bytes.NewReader(bz), int64(len(bz)))
	if err != nil {
		return err
	}

	// zip4.zip contains several "inner" zip files, each of which contains a
	// single txt file.
	for _, f := range zr.File {
		if !regexp.MustCompile(`^zip4mst\d+\.zip$`).MatchString(f.Name) {
			return fmt.Errorf("unexpected entry in zip4 zip: %v", f.Name)
		}

		f.SetPassword(zipPassword)
		r, err := f.Open()
		if err != nil {
			return err
		}
		defer r.Close()

		bzi, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		zri, err := zip.NewReader(bytes.NewReader(bzi), int64(len(bzi)))
		if err != nil {
			return err
		}
		if len(zri.File) != 2 {
			return fmt.Errorf("expected 2 files in zip4 inner zip (found %v)", len(zri.File))
		}

		fi := zri.File[0]
		if !regexp.MustCompile(`^zip4mst\d+\.txt$`).MatchString(fi.Name) {
			return fmt.Errorf("unexpected entry in zip4 inner zip: %v", fi.Name)
		}

		ri, err := fi.Open()
		if err != nil {
			return err
		}
		defer ri.Close()

		if err = ReadZip4File(ri, yield); err != nil {
			return err
		}
	}

	return nil
}

func ReadZip4File(r io.Reader, yield func(Zip4Detail)) error {
	buf := make([]byte, zip4RecordLength)

	for {
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		if buf[0] == Zip4CopyrightDetailCodeDetail[0] {
			detail, err := parseZip4Detail(buf)

			if err != nil {
				return err
			}

			yield(detail)
		}
	}

	return nil
}

func parseZip4Detail(buf []byte) (Zip4Detail, error) {
	s := string(buf)

	var d Zip4Detail
	d.ZipCode = s[1:6]
	d.RecordTypeCode = s[17:18]
	d.Plus4LowNumber = Zip4Number(s[140:144])
	d.Plus4HighNumber = Zip4Number(s[144:148])
	d.StateAbbreviation = s[157:159]
	d.CountyNumber = s[159:162]

	return d, nil
}

func readTarEntry(tarName string, entry string) ([]byte, error) {
	f, err := os.Open(tarName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	tr := tar.NewReader(f)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if header.Name == entry {
			return io.ReadAll(tr)
		}
	}

	return nil, fmt.Errorf("not found: %v", entry)
}
