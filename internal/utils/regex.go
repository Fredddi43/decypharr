package utils

import (
	"path/filepath"
	"regexp"
	"strings"
)

// mediaExtensions is a set of known media file extensions (lowercase, without dot)
var mediaExtensions = map[string]struct{}{
	// Video
	"webm": {}, "m4v": {}, "3gp": {}, "nsv": {}, "ty": {}, "strm": {},
	"rm": {}, "rmvb": {}, "m3u": {}, "ifo": {}, "mov": {}, "qt": {},
	"divx": {}, "xvid": {}, "bivx": {}, "nrg": {}, "pva": {}, "wmv": {},
	"asf": {}, "asx": {}, "ogm": {}, "ogv": {}, "m2v": {}, "avi": {},
	"bin": {}, "dat": {}, "dvr-ms": {}, "mpg": {}, "mpeg": {}, "mp4": {},
	"avc": {}, "vp3": {}, "svq3": {}, "nuv": {}, "viv": {}, "dv": {},
	"fli": {}, "flv": {}, "wpl": {}, "vob": {}, "mkv": {}, "mk3d": {},
	"ts": {}, "wtv": {}, "m2ts": {},
	// Audio
	"mp2": {}, "mp3": {}, "m4a": {}, "m4b": {}, "m4p": {}, "ogg": {},
	"oga": {}, "opus": {}, "wma": {}, "wav": {}, "wv": {}, "flac": {},
	"ape": {}, "aif": {}, "aiff": {}, "aifc": {},
}

func RemoveInvalidChars(value string) string {
	return strings.Map(func(r rune) rune {
		if r == filepath.Separator || r == ':' {
			return r
		}
		if filepath.IsAbs(string(r)) {
			return r
		}
		if strings.ContainsRune(filepath.VolumeName("C:"+string(r)), r) {
			return r
		}
		if r < 32 || strings.ContainsRune(`<>:"/\|?*`, r) {
			return -1
		}
		return r
	}, value)
}

func RemoveExtension(value string) string {
	ext := filepath.Ext(value)
	if ext == "" {
		return value
	}
	// Remove the leading dot and lowercase for lookup
	extLower := strings.ToLower(ext[1:])
	if _, ok := mediaExtensions[extLower]; ok {
		name := value[:len(value)-len(ext)]
		if name != "" && name != "." {
			return name
		}
	}
	return value
}

func IsMediaFile(path string) bool {
	ext := filepath.Ext(path)
	if ext == "" {
		return false
	}
	extLower := strings.ToLower(ext[1:])
	_, ok := mediaExtensions[extLower]
	return ok
}

// defaultExtraFileRegex matches common "extras" found inside multi-file
// season packs and BD-rips — promos, creditless OPs/EDs, menus, samples,
// trailers, interviews, etc. These files are not episodes and skipping
// them when building symlinks keeps Sonarr/Radarr from running ffprobe
// against junk during the import scan.
//
// Match against the basename of the file. Case-insensitive. Compiled
// once at init for performance.
var defaultExtraFileRegex = regexp.MustCompile(`(?i)` +
	// Bracketed tags. Allows arbitrary prefix words inside the bracket
	// (e.g. "[JPN BD Menu 07]", "[PV 06 - CVs]", "[Creditless Ending 01]")
	// but requires a word boundary around the marker keyword to avoid
	// matching things like "[Erai-raws]" against a substring.
	`(\[[^\]]*\b(PV|NCED|NCOP|Creditless|BD[\s_-]?Menu|DVD[\s_-]?Menu|Sample|Trailer|Bonus|Extra|Featurette|Promo|Teaser|Interview)\b[^\]]*\])` +
	// Standalone tag in the filename body. Conservative — only keywords
	// that are virtually never part of a legitimate show/episode title.
	`|(\b(NCED|NCOP|Creditless)\b)` +
	// `*-sample.mkv` and the bare `sample.mkv`.
	`|(-sample\.[a-z0-9]{2,4}$)` +
	`|(^sample\b)`)

// IsExtraFile reports whether the given filename matches any of the
// supplied regex patterns. If patterns is empty, falls back to the
// built-in defaultExtraFileRegex. Compiled regexes are cached per call
// — for tight loops, callers should compile + reuse their own slice.
func IsExtraFile(name string, patterns []string) bool {
	base := filepath.Base(name)
	if len(patterns) == 0 {
		return defaultExtraFileRegex.MatchString(base)
	}
	for _, p := range patterns {
		re, err := regexp.Compile(`(?i)` + p)
		if err != nil {
			continue
		}
		if re.MatchString(base) {
			return true
		}
	}
	return false
}
