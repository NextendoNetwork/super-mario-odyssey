package main

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// nnSdk decodes cdn-image JPEGs into a buffer sized for the requested ImageSize, so match it exactly.
const avatarCacheTTL = 5 * time.Minute

type avatarEntry struct {
	jpeg []byte
	at   time.Time
}

var (
	avatarMu    sync.Mutex
	avatarCache = map[string]avatarEntry{}
	avatarHTTP  = &http.Client{Timeout: 5 * time.Second}
)

func registerAvatarCDN(mux *http.ServeMux) {
	serve := func(w http.ResponseWriter, r *http.Request) {
		size := 128
		if n, err := strconv.Atoi(r.URL.Query().Get("size")); err == nil && n >= 32 && n <= 512 {
			size = n
		}
		thumb := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		pid, err := strconv.ParseUint(r.URL.Query().Get("pid"), 10, 64)
		if err != nil {
			pid, _ = strconv.ParseUint(strings.TrimPrefix(thumb, "pid_"), 10, 64)
		}
		body := avatarJPEG(pid, size)
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Write(body)
	}
	mux.HandleFunc("/1/", serve)
	mux.HandleFunc("/2/", serve)
}

func avatarJPEG(pid uint64, size int) []byte {
	key := fmt.Sprintf("%d/%d", pid, size)
	avatarMu.Lock()
	if e, ok := avatarCache[key]; ok && time.Since(e.at) < avatarCacheTTL {
		avatarMu.Unlock()
		return e.jpeg
	}
	avatarMu.Unlock()

	var src image.Image
	if pid != 0 {
		src = fetchAvatar(pid)
	}
	dst := image.NewRGBA(image.Rect(0, 0, size, size))
	if src == nil {
		for i := range dst.Pix {
			dst.Pix[i] = 0xB4
		}
	} else {
		scaleInto(dst, src)
	}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 90}); err != nil {
		return nil
	}
	avatarMu.Lock()
	avatarCache[key] = avatarEntry{buf.Bytes(), time.Now()}
	avatarMu.Unlock()
	return buf.Bytes()
}

func fetchAvatar(pid uint64) image.Image {
	resp, err := avatarHTTP.Get(fmt.Sprintf("%s/api/avatar?pid=%d", envOr("NEXTENDO_AVATAR_URL", "https://nextendo.network"), pid))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	img, _, err := image.Decode(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil
	}
	return img
}

// scaleInto box-filters src onto dst.
func scaleInto(dst *image.RGBA, src image.Image) {
	sb := src.Bounds()
	dw, dh := dst.Bounds().Dx(), dst.Bounds().Dy()
	for y := 0; y < dh; y++ {
		y0, y1 := sb.Min.Y+y*sb.Dy()/dh, sb.Min.Y+(y+1)*sb.Dy()/dh
		if y1 <= y0 {
			y1 = y0 + 1
		}
		for x := 0; x < dw; x++ {
			x0, x1 := sb.Min.X+x*sb.Dx()/dw, sb.Min.X+(x+1)*sb.Dx()/dw
			if x1 <= x0 {
				x1 = x0 + 1
			}
			var r, g, b, n uint64
			for sy := y0; sy < y1; sy++ {
				for sx := x0; sx < x1; sx++ {
					cr, cg, cb, _ := src.At(sx, sy).RGBA()
					r, g, b, n = r+uint64(cr), g+uint64(cg), b+uint64(cb), n+1
				}
			}
			dst.SetRGBA(x, y, color.RGBA{uint8(r / n >> 8), uint8(g / n >> 8), uint8(b / n >> 8), 0xFF})
		}
	}
}
